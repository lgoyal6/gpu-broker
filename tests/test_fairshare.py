"""Queue ordering: fair share, decay, and the reason a heavy user is not exiled."""

from __future__ import annotations

import pytest

from conftest import settle_history
from gpu_broker.config import load_config
from gpu_broker.errors import ConfigError
from gpu_broker.fairshare import decay_weight, usage_shares
from gpu_broker.money import Currency
from gpu_broker.states import JobState


def queue_users(broker) -> list[str]:
    return [job.user_id for job, _ in broker.queue()]


# ------------------------------------------------------------------- the rule


def test_a_light_user_goes_ahead_of_a_heavy_one_who_submitted_first(broker, users, clock):
    settle_history(broker, "cy", "30.00", days_ago=20)
    broker.submit(user_id="cy", command="x", gpu_type="a10g", hours=1)
    clock.advance(minutes=1)
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)

    assert queue_users(broker) == ["ana", "cy"], "submit order beat fair share"


def test_ordering_is_independent_of_submit_time(broker, users, clock):
    """The same five users, submitted in the opposite order, come out the same."""
    for name, prior in [("ana", "0"), ("bo", "4"), ("cy", "12"), ("di", "20"), ("eo", "31")]:
        if prior != "0":
            settle_history(broker, name, prior, days_ago=20)

    for name in ["eo", "di", "cy", "bo", "ana"]:
        broker.submit(user_id=name, command="x", gpu_type="a10g", hours=1)
        clock.advance(minutes=1)

    assert queue_users(broker) == ["ana", "bo", "cy", "di", "eo"]


def test_usage_decays_so_a_heavy_month_is_not_a_sentence(broker, users, clock):
    settle_history(broker, "cy", "30.00")
    settle_history(broker, "ana", "10.00")

    before = usage_shares(broker.store, clock.now(), broker.config)["cy"]
    clock.advance(days=28)  # two half-lives
    after = usage_shares(broker.store, clock.now(), broker.config)["cy"]

    # Both decay together, so the *share* holds while the magnitude falls.
    # What matters is that absolute usage stops dominating as it ages out.
    from gpu_broker.fairshare import decayed_usage

    assert decayed_usage(broker.store, Currency.USD, clock.now(), broker.config)["cy"] == pytest.approx(
        30.0 * 0.25, rel=1e-6
    )
    assert before == pytest.approx(after, rel=1e-6)


def test_usage_outside_the_window_stops_counting(broker, users, clock):
    settle_history(broker, "cy", "30.00")
    clock.advance(days=31)  # past the 30-day window
    assert usage_shares(broker.store, clock.now(), broker.config) == {}


def test_decay_weight_halves_every_half_life():
    assert decay_weight(0, 14) == 1.0
    assert decay_weight(14, 14) == pytest.approx(0.5)
    assert decay_weight(28, 14) == pytest.approx(0.25)


# ------------------------------------------------------- the starvation guard


def test_a_fully_aged_heavy_job_outranks_a_fresh_light_one(broker, users, clock):
    """The formula property the age term exists to provide.

    Note what this does *not* claim: an aged heavy job does not outrank an
    equally aged light job, and should not. The guarantee is only about jobs
    that keep arriving fresh.
    """
    settle_history(broker, "cy", "40.00", days_ago=20)
    broker.submit(user_id="cy", command="starved", gpu_type="a10g", hours=1)

    early = broker.submit(user_id="ana", command="fresh", gpu_type="a10g", hours=1).job
    assert queue_users(broker)[0] == "ana", "expected the light user to lead early on"

    broker.cancel(early.job_id, actor="ana")
    clock.advance(hours=13)  # cy's job is now past age_max_hours
    broker.submit(user_id="ana", command="fresh-again", gpu_type="a10g", hours=1)

    assert queue_users(broker)[0] == "cy", "the aged heavy job never caught up"


def test_a_heavy_user_is_not_starved_by_a_stream_of_light_arrivals(broker, users, clock, cloud):
    """The failure mode as a simulation, with the scheduler actually running.

    `cy` submits once and then nothing. `ana` submits a fresh job every hour
    forever. Capacity is one slot, so exactly one job can run at a time. If the
    age term did not exist, ana's newest job would win every single dispatch and
    cy would sit there until the semester ended.

    This is the test that would have caught the weights being set wrong; the
    formula test above passes for weights that still starve people in practice.
    """
    cloud.capacity = {"a10g": 1}
    settle_history(broker, "cy", "40.00", days_ago=20)
    cy_job = broker.submit(user_id="cy", command="starved", gpu_type="a10g", hours=0.5).job

    for hour in range(24):
        broker.submit(user_id="ana", command=f"fresh-{hour}", gpu_type="a10g", hours=0.5)
        broker.tick()
        if broker.status(cy_job.job_id).state is not JobState.QUEUED:
            assert hour < 20, f"cy waited {hour}h, which is starvation in practice"
            return
        clock.advance(hours=1)

    pytest.fail("cy never got dispatched in 24 hours of one-slot contention")


def test_the_config_refuses_weights_that_would_starve_someone(state_dir):
    with pytest.raises(ConfigError, match="starved"):
        load_config(state_dir, {"w_fair": 0.9, "w_age": 0.1})


def test_the_age_term_saturates(broker, users, clock):
    """Waiting thirty hours is not three times as urgent as waiting ten. Without
    saturation the age term eventually swamps fair share entirely and the queue
    degenerates to FIFO."""
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    clock.advance(hours=12)
    _, at_max = broker.queue()[0]
    clock.advance(hours=100)
    _, way_past = broker.queue()[0]
    assert at_max.age_term == way_past.age_term == pytest.approx(broker.config.w_age)


# ------------------------------------------------------------- two currencies


def test_free_gpu_hours_count_toward_a_users_share(broker, users, clock):
    """Somebody who lives on the free lab machine has consumed real club
    capacity, and should not also get first pick of the paid pool."""
    settle_history(broker, "cy", "100.00", currency=Currency.GPU_HOUR)

    broker.submit(user_id="cy", command="x", gpu_type="a10g", hours=1)
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)

    assert queue_users(broker) == ["ana", "cy"]


def test_each_currency_is_shared_against_its_own_pool(broker, users, clock):
    """Spending every dollar and spending every free hour weigh differently,
    by w_usd_share and w_gpu_hour_share, not by an invented exchange rate."""
    settle_history(broker, "ana", "50.00", currency=Currency.USD)
    settle_history(broker, "bo", "50.00", currency=Currency.GPU_HOUR)

    shares = usage_shares(broker.store, clock.now(), broker.config)
    total_weight = broker.config.w_usd_share + broker.config.w_gpu_hour_share
    assert shares["ana"] == pytest.approx(broker.config.w_usd_share / total_weight)
    assert shares["bo"] == pytest.approx(broker.config.w_gpu_hour_share / total_weight)


def test_a_user_who_has_used_nothing_has_a_zero_share(broker, users, clock):
    settle_history(broker, "cy", "10.00")
    shares = usage_shares(broker.store, clock.now(), broker.config)
    assert shares.get("ana", 0.0) == 0.0


def test_ordering_is_a_total_order(broker, users, clock):
    """Identical jobs from identical users must not shuffle between calls, or
    `gpu queue` appears to churn while nothing has changed."""
    for name in users:
        broker.submit(user_id=name, command="x", gpu_type="a10g", hours=1)
    first = [job.job_id for job, _ in broker.queue()]
    for _ in range(5):
        assert [job.job_id for job, _ in broker.queue()] == first


def test_priority_explains_itself(broker, users, clock):
    settle_history(broker, "cy", "20.00")
    broker.submit(user_id="cy", command="x", gpu_type="a10g", hours=1)
    clock.advance(hours=6)
    _, priority = broker.queue()[0]

    assert priority.score == pytest.approx(priority.fair_term + priority.age_term)
    assert "score" in priority.explain() and "waited" in priority.explain()
