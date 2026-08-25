"""The transition table is the contract every later phase builds on."""

from __future__ import annotations

import pytest

from gpu_broker.errors import IllegalTransition
from gpu_broker.states import (
    ACTIVE,
    HOLDS_BUDGET,
    LEGAL_TRANSITIONS,
    TERMINAL,
    JobState,
    can_transition,
)


def test_every_state_appears_in_the_table():
    assert set(LEGAL_TRANSITIONS) == set(JobState)


def test_terminal_states_have_no_way_out():
    for state in TERMINAL:
        assert LEGAL_TRANSITIONS[state] == frozenset(), state


def test_every_non_terminal_state_can_reach_a_terminal_one():
    """No state may be a trap. If a job can enter it, a job can leave it."""
    for state in JobState:
        if state in TERMINAL:
            continue
        seen, frontier = {state}, [state]
        while frontier:
            for target in LEGAL_TRANSITIONS[frontier.pop()]:
                if target not in seen:
                    seen.add(target)
                    frontier.append(target)
        assert seen & TERMINAL, f"{state} cannot reach any terminal state"


def test_every_state_is_reachable_from_submission():
    """Dead states are dead code. QUEUED and REFUSED are the two entry points."""
    seen, frontier = {JobState.QUEUED, JobState.REFUSED}, [JobState.QUEUED]
    while frontier:
        for target in LEGAL_TRANSITIONS[frontier.pop()]:
            if target not in seen:
                seen.add(target)
                frontier.append(target)
    assert seen == set(JobState), f"unreachable: {sorted(set(JobState) - seen)}"


def test_active_states_are_exactly_those_holding_backend_capacity():
    assert ACTIVE == {
        JobState.ALLOCATING,
        JobState.RUNNING,
        JobState.CHECKPOINTING,
        JobState.RESUMING,
    }


def test_anything_holding_capacity_is_also_holding_budget():
    assert ACTIVE <= HOLDS_BUDGET


@pytest.mark.parametrize(
    "source,target",
    [
        (JobState.QUEUED, JobState.RUNNING),      # cannot skip allocation
        (JobState.QUEUED, JobState.COMPLETED),    # cannot finish without running
        (JobState.COMPLETED, JobState.RUNNING),   # cannot resurrect
        (JobState.CANCELLED, JobState.QUEUED),
        (JobState.REFUSED, JobState.QUEUED),      # a refusal is not a queue entry
    ],
)
def test_illegal_edges_stay_illegal(source, target):
    assert not can_transition(source, target)


def test_store_refuses_an_illegal_transition(broker, users):
    job = broker.submit(
        user_id="ana", command="x", gpu_type="a10g", hours=1
    ).job
    with pytest.raises(IllegalTransition) as caught:
        broker.store.transition(job, JobState.COMPLETED)
    assert "QUEUED -> COMPLETED" in str(caught.value)
    assert broker.status(job.job_id).state is JobState.QUEUED


def test_a_lost_race_is_reported_as_an_illegal_transition(broker, users):
    """Two schedulers must not both dispatch one job.

    The UPDATE is guarded on the state we read, so the second writer's rowcount
    is zero and it fails loudly rather than double-launching.
    """
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.store.transition(job, JobState.ALLOCATING, backend="cloud", backend_handle="h1")
    # `job` is the stale read the second scheduler would be holding.
    with pytest.raises(IllegalTransition):
        broker.store.transition(job, JobState.ALLOCATING, backend="cloud", backend_handle="h2")
