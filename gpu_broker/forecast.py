"""At the current burn rate, when does the pool run dry.

The number the club actually needs, and the one nobody has today. It is a
straight-line extrapolation and says so: a fortnight of quiet followed by three
people starting week-long runs will not have been predicted by this, and no
arithmetic over past spend would have predicted it.

What it is good for is the case that actually happens -- steady spend nobody is
watching -- and for that a straight line is enough to raise a hand a week early.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal

from .clock import Clock
from .config import BrokerConfig
from .money import ZERO, Currency, fmt, money


@dataclass(frozen=True)
class Forecast:
    currency: Currency
    budget: Decimal
    spent: Decimal
    committed: Decimal
    available: Decimal
    burn_per_day: Decimal
    days_left: float | None
    """None when nothing is being spent, which is not the same as 'plenty of
    time' and should not be rendered as a large number."""
    dry_on: dt.datetime | None
    level: str
    window_days: float
    observed_days: float
    """How much history the burn rate is actually based on. An hour of data
    extrapolated to a month is a guess wearing a number's clothes."""

    @property
    def confident(self) -> bool:
        return self.observed_days >= 1.0

    def headline(self) -> str:
        if self.level == "EXHAUSTED":
            return f"the pool is out of {_noun(self.currency)}"
        if self.days_left is None:
            return f"nothing is being spent; {fmt(self.available, self.currency)} left"
        # Past a year the number is arithmetic rather than information, and a
        # four-digit runway makes the whole line look unserious.
        if self.days_left > 365:
            days = "over a year"
        else:
            days = f"{self.days_left:.0f} day{'s' if round(self.days_left) != 1 else ''}"
        hedge = "" if self.confident else " (based on under a day of history)"
        lead = "about " if not days.startswith("over") else ""
        return (
            f"{fmt(self.available, self.currency)} left, "
            f"{fmt(self.burn_per_day, self.currency)}/day, "
            f"{lead}{days} of runway{hedge}"
        )


def _noun(currency: Currency) -> str:
    return "credit" if currency is Currency.USD else "free GPU-hours"


def forecast(
    store, config: BrokerConfig, clock: Clock, currency: Currency = Currency.USD
) -> Forecast:
    now = clock.now()
    window = config.forecast_window_days
    cutoff = now - dt.timedelta(days=window)

    settlements = store.settlements_since(cutoff, currency)
    burned = sum((amount for _, amount, _ in settlements), ZERO)

    # Measure over the span we actually have data for, not over the nominal
    # window. A broker installed yesterday would otherwise divide a day of
    # spending by thirty and report a burn rate near zero.
    if settlements:
        earliest = min(at for _, _, at in settlements)
        observed = max((now - earliest).total_seconds() / 86400.0, 0.0)
    else:
        observed = 0.0

    effective = max(observed, config.forecast_min_days)
    burn_per_day = money(burned) / money(effective) if burned > ZERO else ZERO

    pool = store.pool_balance(currency)
    committed = store.pool_committed(currency)
    available = pool.budget - committed

    if available <= ZERO:
        level, days_left, dry_on = "EXHAUSTED", 0.0, now
    elif burn_per_day <= ZERO:
        level, days_left, dry_on = "OK", None, None
    else:
        days_left = float(available / burn_per_day)
        dry_on = now + dt.timedelta(days=days_left)
        if days_left <= config.forecast_critical_days:
            level = "CRITICAL"
        elif days_left <= config.forecast_warning_days:
            level = "WARNING"
        else:
            level = "OK"

    return Forecast(
        currency=currency,
        budget=pool.budget,
        spent=pool.spent,
        committed=committed,
        available=available,
        burn_per_day=burn_per_day,
        days_left=days_left,
        dry_on=dry_on,
        level=level,
        window_days=window,
        observed_days=observed,
    )


def alerts(store, config: BrokerConfig, clock: Clock) -> list[Forecast]:
    """Every currency whose runway has fallen past a threshold."""
    out = []
    for currency in Currency:
        result = forecast(store, config, clock, currency)
        if result.level in ("WARNING", "CRITICAL", "EXHAUSTED"):
            out.append(result)
    return out
