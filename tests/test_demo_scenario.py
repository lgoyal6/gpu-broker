"""Seeded demo data.

The scenario runs the real broker against a fake backend, so these tests are
also the integration test for the lifecycle: `states_covered` says which states
a month of ordinary use actually reaches.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from gpu_broker.demo import SEEDED, seed
from gpu_broker.demo.scenario import MARKER
from gpu_broker.states import JobState

# CHECKPOINTING is declared in the lifecycle and never entered by any code path
# in the broker: the spot handler terminates, collects whatever was saved, and
# goes straight to PREEMPTED. The scenario cannot reach a state nothing sets, so
# it is named here rather than quietly excluded. If somebody wires up the
# interruption window, this test fails and tells them to delete the exception.
NEVER_SET = {str(JobState.CHECKPOINTING)}


@pytest.fixture(scope="module")
def seeded(tmp_path_factory) -> "object":
    return seed(tmp_path_factory.mktemp("demo") / "data", days=8, rng_seed=3)


def test_it_reaches_every_state_the_lifecycle_can_actually_enter(seeded):
    assert seeded.missing_states == NEVER_SET, (
        "a lifecycle state the scenario no longer exercises, or one that became "
        "reachable and should be dropped from NEVER_SET"
    )


def test_the_states_it_covers_are_the_interesting_ones(seeded):
    for state in ("COMPLETED", "FAILED", "CANCELLED", "REFUSED", "RECLAIMED", "PREEMPTED", "RESUMING"):
        assert state in seeded.states_covered


def test_it_produces_a_pool_with_more_than_one_person_on_it(seeded):
    assert seeded.users >= 5
    assert seeded.jobs >= 10


def test_every_seeded_job_is_tagged_as_seeded(seeded):
    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock
    from gpu_broker.config import load_config

    with Broker.open(
        seeded.directory, clock=SystemClock(), backends=[], config=load_config(seeded.directory)
    ) as broker:
        origins = {job.origin for job in broker.store.list_jobs(limit=None)}
    assert origins == {SEEDED}


def test_demo_data_lives_in_its_own_database(seeded, tmp_path: Path):
    """Not a flag on a shared table: a different file."""
    from gpu_broker.broker import Broker
    from gpu_broker.clock import ManualClock

    real = tmp_path / "real"
    with Broker.open(real, clock=ManualClock(), backends=[]) as broker:
        broker.add_user("ana")
        assert broker.store.list_jobs(limit=None) == []

    assert seeded.directory.resolve() != real.resolve()
    assert (seeded.directory / MARKER).exists()
    assert not (real / MARKER).exists()


def test_it_refuses_to_seed_over_something_that_might_be_real(tmp_path: Path):
    victim = tmp_path / "probably-real"
    victim.mkdir()
    (victim / "broker.sqlite3").write_bytes(b"not really a database")

    with pytest.raises(RuntimeError, match="Refusing"):
        seed(victim, days=2)

    assert (victim / "broker.sqlite3").read_bytes() == b"not really a database"


def test_it_refuses_to_reseed_without_being_asked_twice(tmp_path: Path):
    target = tmp_path / "demo"
    seed(target, days=2)
    with pytest.raises(RuntimeError, match="already seeded"):
        seed(target, days=2)
    seed(target, days=2, overwrite=True)  # explicit, so allowed


def test_the_same_seed_gives_the_same_history(tmp_path: Path):
    one = seed(tmp_path / "a", days=4, rng_seed=11)
    two = seed(tmp_path / "b", days=4, rng_seed=11)
    assert one.jobs == two.jobs
    assert one.states_covered == two.states_covered


def test_the_reclaimed_job_kept_the_samples_that_justified_it(seeded):
    """The person whose job died has to be able to audit why."""
    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock
    from gpu_broker.config import load_config

    with Broker.open(
        seeded.directory, clock=SystemClock(), backends=[], config=load_config(seeded.directory)
    ) as broker:
        reclaimed = [
            job
            for job in broker.store.list_jobs(limit=None)
            if job.state is JobState.RECLAIMED
        ]
        assert reclaimed, "the scenario is supposed to include an idle reclaim"
        for job in reclaimed:
            assert broker.store.samples_for(job.job_id, limit=500)
            assert broker.store.notifications_for_job(job.job_id, kind="reclaimed")
