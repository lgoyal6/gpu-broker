"""Environment locality: a tie-break, and the things that must never break it.

The lab pool already decides where a job goes by free capacity and then by
hostname. This adds one step between those two: when two hosts are tied for
emptiest, the one that already has the job's environment built wins, because the
job it would otherwise go to pays minutes to build the same virtualenv again.

Most of the tests here exist to prove the step is *only* that. An attractive
locality signal that is wrong -- a different digest, a half-finished build, an
observation from an hour ago, a host that has stopped answering -- has to lose to
the ordinary rules, because the cost of following it is a job that runs on a
busier card, or does not run at all.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from conftest import local_job
from gpu_broker.backends.local import LocalBackend
from gpu_broker.environments import Environment
from gpu_broker.local.hosts import LocalConfig
from gpu_broker.local.locality import EnvironmentFacts, prefer_warm
from ssh_host import FakeGpuHost, SshHostServer

# Two names that both reach this machine, so the pool sees two hosts. "127.0.0.1"
# sorts before "localhost", which makes it the one the existing hostname
# tie-break picks -- so a test where "localhost" wins is a test where locality
# actually changed the answer.
FIRST = "127.0.0.1"
SECOND = "localhost"

ENVIRONMENT = Environment(name="vision", requirements=("torch==2.3.0",))
OTHER = Environment(name="audio", requirements=("librosa==0.10.1",))


@pytest.fixture
def pool(clock, transport):
    """Two lab hosts of equal size behind one backend."""
    machines = {}
    servers = []
    for name in (FIRST, SECOND):
        machine = FakeGpuHost(hostname=name)
        server = SshHostServer(machine)
        machine.server = server
        machines[name] = machine
        servers.append(server)

    def backend_with(**kwargs):
        config = LocalConfig(
            hosts=tuple(machines[name].server.spec(name) for name in (FIRST, SECOND)),
            max_jobs_per_gpu=2,
            command_timeout_seconds=3.0,
            poll_timeout_seconds=2.0,
        )
        return LocalBackend(
            clock=clock,
            config=config,
            transport=transport,
            environments={"vision": ENVIRONMENT, "audio": OTHER}.get,
            **kwargs,
        )

    yield machines, backend_with
    for server in servers:
        server.close()


def env_job(job_id: str, user_id: str = "ana", environment: str | None = "vision"):
    return replace(local_job(job_id, user_id), environment=environment)


def ready(machine: FakeGpuHost, environment: Environment) -> None:
    """The finished environment, at the path the build moves into place."""
    machine.environment_dirs[environment.digest] = True


def placed_on(backend: LocalBackend, job) -> str:
    return backend.launch(job).handle.split("/", 1)[0]


def place_and_release(machines, backend: LocalBackend, job) -> str:
    """Place a job, then finish it so it gives its slot back.

    Needed wherever a test places twice and wants the *second* placement to turn
    on locality. A job left running makes its host one slot busier, and a busier
    host loses to free capacity before locality is ever consulted -- so without
    this the test would pass for the wrong reason and would keep passing with the
    tie-break deleted.
    """
    hostname = placed_on(backend, job)
    machines[hostname].finish(job.job_id, 0)
    return hostname


# ------------------------------------------------------------ the tie-break


def test_a_tie_goes_to_the_host_that_already_has_the_environment(pool):
    """The whole point. Both hosts are equally empty, so the job is going to one
    of them either way; sending it to the one with the virtualenv already built
    saves it a pip install it would otherwise pay for in full."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()

    assert placed_on(backend, env_job("j1")) == SECOND


def test_the_emptiest_host_still_wins_over_a_warm_busier_one(pool):
    """Locality is worth one build. A busier card is worth every hour the job
    then spends sharing it, so free capacity is not something this may trade."""
    machines, backend_with = pool
    machines[FIRST].gpus = [
        (0, "NVIDIA RTX A6000", 49140, 512, 3, "Default"),
        (1, "NVIDIA RTX A6000", 49140, 512, 3, "Default"),
    ]
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()

    assert placed_on(backend, env_job("j1")) == FIRST


def test_it_does_not_even_ask_when_capacity_already_decided(pool):
    """A probe that cannot change the answer is a round trip nobody needed."""
    machines, backend_with = pool
    machines[FIRST].gpus = [
        (0, "NVIDIA RTX A6000", 49140, 512, 3, "Default"),
        (1, "NVIDIA RTX A6000", 49140, 512, 3, "Default"),
    ]
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()

    backend.launch(env_job("j1"))
    assert machines[SECOND].environment_probes == []


def test_a_job_with_no_environment_is_placed_exactly_as_before(pool):
    """No environment, no digest, nothing to be local to. The job has to follow
    the old path, and it has to do it without paying for a probe first."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()

    assert placed_on(backend, env_job("j1", environment=None)) == FIRST
    assert machines[FIRST].environment_probes == []
    assert machines[SECOND].environment_probes == []


def test_two_equally_warm_hosts_still_break_the_tie_by_hostname(pool):
    """Determinism survives. Two hosts that are equally empty and equally warm
    have to land the same way every run, or the same submission stops being
    reproducible."""
    machines, backend_with = pool
    ready(machines[FIRST], ENVIRONMENT)
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()

    assert placed_on(backend, env_job("j1")) == FIRST


def test_an_unknown_environment_name_does_not_break_placement(pool):
    """The job names something the broker has never heard of. There is no digest
    to be local to, so this is the old path, not an error."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()

    assert placed_on(backend, env_job("j1", environment="does-not-exist")) == FIRST


# ------------------------------------------------- the negative controls
#
# Each of these is an attractive locality signal that is not a fact. If any of
# them can move a job, the tie-break has become a way to place work badly.


def test_a_different_digest_on_the_warm_host_is_ignored(pool):
    """Somebody else's environment is not this job's environment. Reusing it
    would run the job with the wrong packages and no error, which is the exact
    failure content addressing exists to prevent."""
    machines, backend_with = pool
    ready(machines[SECOND], OTHER)
    backend = backend_with()

    assert placed_on(backend, env_job("j1")) == FIRST
    assert machines[SECOND].environment_probes == [ENVIRONMENT.digest]


def test_a_half_finished_build_is_ignored(pool):
    """`venv_script` installs into `.building-<digest>` and moves the tree into
    place at the very end. The staging directory has a real interpreter in it, so
    a check that asked "is there a python here" about the wrong path would say
    yes and hand the job a virtualenv that is still being written."""
    machines, backend_with = pool
    machines[SECOND].environment_dirs[f".building-{ENVIRONMENT.digest}"] = True
    backend = backend_with()

    assert placed_on(backend, env_job("j1")) == FIRST


def test_a_directory_with_no_interpreter_is_ignored(pool):
    """A failed build can leave the name behind without leaving anything that
    runs. The probe tests `bin/python` for executability precisely so that the
    leftover does not read as a cache hit."""
    machines, backend_with = pool
    machines[SECOND].environment_dirs[ENVIRONMENT.digest] = False
    backend = backend_with()

    assert placed_on(backend, env_job("j1")) == FIRST


def test_a_missing_environment_is_ignored(pool):
    machines, backend_with = pool
    backend = backend_with()

    assert placed_on(backend, env_job("j1")) == FIRST


def test_an_expired_observation_is_re_checked_not_believed(pool, clock):
    """Facts go stale on their own: somebody clears a virtualenv, a disk fills, a
    host is reimaged. An observation past its expiry is not evidence, so the
    broker asks again rather than acting on what used to be true."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()
    assert place_and_release(machines, backend, env_job("j1")) == SECOND

    machines[SECOND].environment_dirs.clear()  # the environment goes away
    clock.advance(seconds=600)  # past both the fact TTL and the health interval

    assert placed_on(backend, env_job("j2")) == FIRST


def test_a_fresh_observation_is_not_re_probed(pool, clock):
    """The cache is what keeps this bounded: one probe per host per digest per
    lifetime, not one per placement."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()

    place_and_release(machines, backend, env_job("j1"))
    place_and_release(machines, backend, env_job("j2"))
    assert machines[SECOND].environment_probes == [ENVIRONMENT.digest]


def test_a_probe_error_leaves_ordinary_placement_working(pool):
    """A host that cannot answer the locality question is a host that does not
    win the tie. It is never a host that gets held back from a job it could
    otherwise run, and the failure never reaches the caller."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    machines[SECOND].environment_probe_works = False
    backend = backend_with()

    assert placed_on(backend, env_job("j1")) == FIRST


def test_a_probe_error_is_not_remembered_as_an_answer(pool):
    """Caching a failed probe would spend the whole fact lifetime acting on one
    bad round trip, and the host would stay cold long after it recovered."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    machines[SECOND].environment_probe_works = False
    backend = backend_with()
    place_and_release(machines, backend, env_job("j1"))

    machines[SECOND].environment_probe_works = True
    assert placed_on(backend, env_job("j2")) == SECOND


def test_a_warm_host_that_stops_answering_loses_to_a_cold_healthy_one(pool, clock):
    """Health comes first and locality comes last, so a warm host that has gone
    away cannot pull work onto itself. The cold host that is actually up wins."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()
    assert place_and_release(machines, backend, env_job("j1")) == SECOND

    machines[SECOND].server.close()
    clock.advance(seconds=200)  # past the health interval, so the host is re-checked

    assert placed_on(backend, env_job("j2")) == FIRST


def test_a_warm_host_that_is_drained_loses_to_a_cold_healthy_one(pool):
    """The same ordering, for the drain a human asked for rather than one a
    failure caused."""
    machines, backend_with = pool
    ready(machines[SECOND], ENVIRONMENT)
    backend = backend_with()
    backend.drain(SECOND, "swapping a fan")

    assert placed_on(backend, env_job("j1")) == FIRST


# ------------------------------------------------------- the facts themselves


def test_a_fact_carries_nothing_but_the_observation(clock):
    """Digest, host, what was seen, when, and until when. A fact that also
    carried a weight, a command or a user would be a policy in disguise, and the
    policy in this file is the ordering, which is readable in one place."""
    facts = EnvironmentFacts()
    fact = facts.record("lab1", "abc123", True, clock.now())

    assert set(vars(fact)) == {
        "hostname",
        "digest",
        "materialized",
        "observed_at",
        "expires_at",
    }


def test_an_expired_fact_is_dropped_rather_than_returned(clock):
    facts = EnvironmentFacts(ttl_seconds=60)
    facts.record("lab1", "abc123", True, clock.now())
    clock.advance(seconds=61)

    assert facts.get("lab1", "abc123", clock.now()) is None
    assert len(facts) == 0


def test_recording_clears_out_facts_that_stopped_being_worth_anything(clock):
    """What keeps the cache bounded is that entries expire, not a size limit
    somebody has to pick a number for."""
    facts = EnvironmentFacts(ttl_seconds=60)
    facts.record("lab1", "abc123", True, clock.now())
    clock.advance(seconds=61)
    facts.record("lab2", "def456", True, clock.now())

    assert len(facts) == 1


# --------------------------------------------------------- the ordering rule


def test_the_ordering_only_ever_reaches_into_the_first_group():
    """`prefer_warm` is handed the existing order and may pick a different
    element of the emptiest group. Everything else is the ordering it was
    given."""
    ranked = [(4, "a"), (2, "b"), (2, "c")]
    assert prefer_warm(ranked, lambda name: name == "c") == "a"


def test_the_ordering_falls_through_when_nothing_is_warm():
    ranked = [(2, "a"), (2, "b")]
    assert prefer_warm(ranked, lambda name: False) == "a"


def test_the_ordering_falls_through_with_no_environment():
    ranked = [(2, "a"), (2, "b")]
    assert prefer_warm(ranked, None) == "a"


def test_the_ordering_never_asks_about_a_sole_candidate():
    asked = []

    def is_warm(name):
        asked.append(name)
        return True

    assert prefer_warm([(2, "a")], is_warm) == "a"
    assert asked == []
