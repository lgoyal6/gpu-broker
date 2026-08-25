"""Which capacity a job lands on, and whether it can say why."""

from __future__ import annotations

from dataclasses import replace

import pytest

from gpu_broker.backends import FakeBackend
from gpu_broker.money import Currency
from gpu_broker.placement import choose, validate_order
from gpu_broker.errors import BrokerError
from gpu_broker.states import JobState


@pytest.fixture
def free(clock, state_dir):
    """A free tier, billed in GPU-hours."""
    return FakeBackend(
        "lab", clock=clock, currency=Currency.GPU_HOUR,
        capacity={"a6000": 2}, tier="local", state_path=state_dir / "lab.json",
    )


@pytest.fixture
def paid(clock, state_dir):
    return FakeBackend(
        "cloud", clock=clock, currency=Currency.USD,
        capacity={"a10g": 2}, tier="ondemand", state_path=state_dir / "cloud.json",
    )


def usd_job(job_id="j1", user_id="ana", gpu_type="a10g"):
    from decimal import Decimal
    import datetime as dt
    from gpu_broker.models import Job

    now = dt.datetime(2026, 1, 15, 12, tzinfo=dt.timezone.utc)
    return Job(
        job_id=job_id.ljust(32, "0"), user_id=user_id, command="python train.py",
        gpu_type=gpu_type, requested_hours=2, currency=Currency.USD,
        reserved=Decimal("4"), state=JobState.QUEUED, submitted_at=now, updated_at=now,
    )


def test_free_capacity_is_preferred(config, free, paid):
    free_a10g = FakeBackend(
        "lab", clock=free.clock, currency=Currency.USD, capacity={"a10g": 1}, tier="local"
    )
    result = choose(usd_job(), [paid, free_a10g], config)
    assert result.placed
    assert result.backend.name == "lab"
    assert result.tier == "local"


def test_order_beats_registration_order(config, free, paid):
    """The policy decides, not whichever backend happened to be constructed first."""
    free_a10g = FakeBackend(
        "lab", clock=free.clock, currency=Currency.USD, capacity={"a10g": 1}, tier="local"
    )
    assert choose(usd_job(), [paid, free_a10g], config).tier == "local"
    assert choose(usd_job(), [free_a10g, paid], config).tier == "local"


def test_a_full_free_tier_falls_through_to_paid_immediately(config, paid, clock):
    """The chosen policy: take whatever is free right now, never wait for
    cheaper capacity. That trades money for latency on purpose."""
    full = FakeBackend(
        "lab", clock=clock, currency=Currency.USD, capacity={"a10g": 0}, tier="local"
    )
    result = choose(usd_job(), [full, paid], config)
    assert result.backend.name == "cloud"
    assert result.tier == "ondemand"
    assert ("lab", "no free a10g right now") in result.skipped


def test_a_decision_records_what_it_skipped_and_why(config, paid, clock):
    full = FakeBackend("lab", clock=clock, currency=Currency.USD, capacity={"a10g": 0}, tier="local")
    result = choose(usd_job(), [full, paid], config)

    assert "skipped lab" in result.explain()
    assert "no free a10g right now" in result.explain()
    assert "placed on cloud [ondemand]" in result.one_line()


def test_a_backend_in_the_wrong_currency_is_skipped_not_used(config, free, paid):
    """A GPU-hour is not a dollar. Placing a dollar job on the free pool would
    bill nobody for it."""
    result = choose(usd_job(), [free, paid], config)
    assert result.backend.name == "cloud"
    assert any("bills in GPU_HOUR" in why for _, why in result.skipped)


def test_nothing_free_anywhere_is_reported_not_raised(config, clock):
    full = FakeBackend("cloud", clock=clock, currency=Currency.USD, capacity={"a10g": 0}, tier="ondemand")
    result = choose(usd_job(), [full], config)
    assert not result.placed
    assert "nothing has a free a10g right now" in result.reason


def test_a_backend_outside_every_configured_tier_is_reported(config, clock):
    stray = FakeBackend("odd", clock=clock, currency=Currency.USD, capacity={"a10g": 2}, tier="mystery")
    result = choose(usd_job(), [stray], replace(config, placement_order=("local", "ondemand")))
    assert not result.placed
    assert "no backend is in any configured tier" in result.reason
    assert "mystery" in result.reason


def test_a_backend_with_no_tier_is_treated_as_expensive(config, clock):
    """Defaulting a misconfigured backend into the free tier would have it
    picked first and quietly spend money the policy meant to defer."""
    from gpu_broker.placement import _tier_of

    class Bare:
        name = "bare"

    assert _tier_of(Bare()) == "ondemand"


def test_validate_order_catches_a_backend_that_could_never_be_used(config, free, paid):
    with pytest.raises(BrokerError, match="would never be used"):
        validate_order(("ondemand",), [free, paid])


def test_the_order_is_configurable(config, clock, paid):
    """Somebody who would rather burn credits than queue can say so."""
    lab = FakeBackend("lab", clock=clock, currency=Currency.USD, capacity={"a10g": 1}, tier="local")
    reversed_order = replace(config, placement_order=("ondemand", "local"))
    assert choose(usd_job(), [lab, paid], reversed_order).backend.name == "cloud"


def test_the_reasoning_lands_on_the_job(broker, users, clock, cloud):
    """When somebody asks why their job cost money, the answer is on their own
    job's log rather than in a scheduler that has moved on."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.tick()

    lines = [line for _, _, _, line in broker.logs(job.job_id)]
    assert any(line.startswith("placement:") for line in lines), lines
    assert any("first tier in the policy" in line for line in lines)
