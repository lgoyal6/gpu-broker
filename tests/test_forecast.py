"""Runway: at the current burn rate, when does the pool run dry."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from conftest import settle_history
from gpu_broker.forecast import alerts, forecast
from gpu_broker.money import Currency


def spend(broker, user_id, amount, days_ago=0.0):
    settle_history(broker, user_id, amount, days_ago=days_ago, note="usage")


def test_an_untouched_pool_has_no_burn_rate(broker, users):
    result = forecast(broker.store, broker.config, broker.clock)
    assert result.burn_per_day == Decimal("0")
    assert result.days_left is None
    assert result.level == "OK"
    assert "nothing is being spent" in result.headline()


def test_a_zero_burn_rate_is_not_rendered_as_infinite_runway(broker, users):
    """`days_left is None` and 'plenty of time' are different claims."""
    result = forecast(broker.store, broker.config, broker.clock)
    assert result.days_left is None
    assert "999" not in result.headline()


def test_runway_is_available_over_burn_rate(broker, users, clock):
    spend(broker, "ana", "100.00", days_ago=10)
    result = forecast(broker.store, broker.config, broker.clock)

    assert result.burn_per_day == pytest.approx(Decimal("10.00"), abs=Decimal("0.5"))
    # $500 pool, $100 gone, $400 left at $10/day.
    assert result.days_left == pytest.approx(40, abs=2)


def test_the_burn_rate_uses_the_history_that_exists_not_the_window(broker, users):
    """A broker installed yesterday would otherwise divide a day of spending by
    thirty and report a burn rate near zero."""
    spend(broker, "ana", "60.00", days_ago=2)
    result = forecast(broker.store, broker.config, broker.clock)

    assert result.observed_days == pytest.approx(2, abs=0.1)
    assert result.burn_per_day == pytest.approx(Decimal("30.00"), abs=Decimal("1"))


def test_very_little_history_is_flagged_rather_than_extrapolated_silently(broker, users, clock):
    spend(broker, "ana", "5.00", days_ago=0.05)
    result = forecast(broker.store, broker.config, broker.clock)

    assert not result.confident
    assert "under a day of history" in result.headline()


def test_the_minimum_period_stops_an_hour_becoming_a_catastrophe(broker, users):
    """Without a floor, an hour of spending divided by an hour of history
    projects the pool empty by lunchtime."""
    spend(broker, "ana", "5.00", days_ago=0.01)
    result = forecast(broker.store, broker.config, broker.clock)
    assert result.burn_per_day <= Decimal("20.00")


@pytest.mark.parametrize(
    "burn,expected",
    [("10.00", "OK"), ("40.00", "WARNING"), ("80.00", "CRITICAL")],
)
def test_thresholds(broker, users, burn, expected):
    spend(broker, "ana", burn, days_ago=1)
    assert forecast(broker.store, broker.config, broker.clock).level == expected


def test_an_exhausted_pool_says_so(broker, users, state_dir, clock, cloud, lab):
    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    tight = Broker.open(
        state_dir, clock=clock, backends=[cloud, lab],
        config=replace(load_config(state_dir), pool_budget_usd=Decimal("10.00")),
    )
    for name in users:
        tight.add_user(name)
    settle_history(tight, "ana", "10.00")

    result = forecast(tight.store, tight.config, tight.clock)
    assert result.level == "EXHAUSTED"
    assert result.days_left == 0.0
    assert "out of credit" in result.headline()
    tight.close()


def test_committed_capacity_counts_against_the_runway(broker, users, clock):
    """Money held by running jobs is money that is going to be gone."""
    before = forecast(broker.store, broker.config, broker.clock).available
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="12")
    broker.tick()
    after = forecast(broker.store, broker.config, broker.clock).available
    assert after < before


def test_free_gpu_hours_are_forecast_separately(broker, users):
    settle_history(broker, "ana", "40.00", currency=Currency.GPU_HOUR, days_ago=1)

    dollars = forecast(broker.store, broker.config, broker.clock, Currency.USD)
    hours = forecast(broker.store, broker.config, broker.clock, Currency.GPU_HOUR)

    assert dollars.burn_per_day == Decimal("0")
    assert hours.burn_per_day > Decimal("0")
    assert "gpu-hr" in hours.headline()
    assert "$" not in hours.headline()


def test_alerts_only_fire_past_a_threshold(broker, users):
    assert alerts(broker.store, broker.config, broker.clock) == []
    spend(broker, "ana", "80.00", days_ago=1)
    fired = alerts(broker.store, broker.config, broker.clock)
    assert [f.level for f in fired] == ["CRITICAL"]


def test_the_forecast_says_which_day(broker, users, clock):
    spend(broker, "ana", "40.00", days_ago=1)
    result = forecast(broker.store, broker.config, broker.clock)
    assert result.dry_on is not None
    assert result.dry_on > clock.now()
