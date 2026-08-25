"""Who goes next, and why.

    priority = w_fair * (1 - usage_share) + w_age * min(1, wait / age_max)

The first term is the fair-share half: a user who has consumed less of the pool
this month sorts ahead of a heavy user, regardless of who submitted first. The
second term is the reason the first one is survivable: without it, a heavy user
loses to every newly-arriving light user forever, and in a twenty-person club
that is one person who stops using the broker.

Prior usage decays with a half-life, so a heavy month fades instead of becoming
a permanent sentence.

Both currencies feed one share. A user who lives on the free lab machine has
consumed real club capacity and should not also get first pick of the paid pool.
The blend weights say how much each currency counts.
"""

from __future__ import annotations

import datetime as dt

from .config import BrokerConfig
from .models import Job, Priority
from .money import Currency


def decay_weight(age_days: float, half_life_days: float) -> float:
    """How much a settlement from `age_days` ago still counts. 1.0 at zero age."""
    if age_days <= 0:
        return 1.0
    return 0.5 ** (age_days / half_life_days)


def decayed_usage(
    store, currency: Currency, now: dt.datetime, config: BrokerConfig
) -> dict[str, float]:
    """Decayed consumption per user, in one currency, over the window."""
    cutoff = now - dt.timedelta(days=config.fairshare_window_days)
    totals: dict[str, float] = {}
    for user_id, amount, at in store.settlements_since(cutoff, currency):
        age_days = (now - at).total_seconds() / 86400.0
        weight = decay_weight(age_days, config.fairshare_half_life_days)
        totals[user_id] = totals.get(user_id, 0.0) + float(amount) * weight
    return totals


def usage_shares(
    store, now: dt.datetime, config: BrokerConfig
) -> dict[str, float]:
    """Each user's blended share of everything consumed in the window, in [0, 1].

    Computed per currency and then blended, because dollars and GPU-hours have
    no exchange rate. A user who spent half the club's dollars and none of its
    free hours lands at `w_usd_share * 0.5`, not at some invented dollar total.
    """
    blended: dict[str, float] = {}
    weight_total = 0.0

    for currency in Currency:
        blend_weight = config.share_weight(currency)
        if blend_weight <= 0:
            continue
        weight_total += blend_weight

        usage = decayed_usage(store, currency, now, config)
        pool_total = sum(usage.values())
        if pool_total <= 0:
            # Nobody has used this currency yet. Everyone's share is zero, which
            # is correct: it should contribute nothing to the ordering.
            continue
        for user_id, amount in usage.items():
            blended[user_id] = blended.get(user_id, 0.0) + blend_weight * (
                amount / pool_total
            )

    if weight_total <= 0:
        return {}
    return {user_id: share / weight_total for user_id, share in blended.items()}


def priority_of(
    job: Job, shares: dict[str, float], now: dt.datetime, config: BrokerConfig
) -> Priority:
    share = shares.get(job.user_id, 0.0)
    wait_hours = job.wait_hours(now)

    fair_term = config.w_fair * (1.0 - share)
    age_term = config.w_age * min(1.0, wait_hours / config.age_max_hours)

    return Priority(
        job_id=job.job_id,
        user_id=job.user_id,
        score=fair_term + age_term,
        fair_term=fair_term,
        age_term=age_term,
        usage_share=share,
        wait_hours=wait_hours,
    )


def order_queue(
    jobs: list[Job], store, now: dt.datetime, config: BrokerConfig
) -> list[tuple[Job, Priority]]:
    """Highest priority first. Ties break on submit time, then job id.

    The tiebreak matters more than it looks: without a total order, two ticks
    could dispatch the same-priority jobs in different orders and `gpu queue`
    would appear to shuffle while nothing changed.
    """
    shares = usage_shares(store, now, config)
    scored = [(job, priority_of(job, shares, now, config)) for job in jobs]
    scored.sort(key=lambda pair: (-pair[1].score, pair[0].submitted_at, pair[0].job_id))
    return scored
