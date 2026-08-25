"""Two currencies, held honestly.

Cloud capacity costs dollars. The lab A6000 costs nothing and is scheduled by
GPU-hours instead. These are not convertible: there is no exchange rate between
"the club's AWS credits" and "the hours nobody else is using the lab machine".

So the ledger carries a currency on every row, budgets come in pairs, and
fair-share computes a share per currency and blends them. The alternative --
shadow-pricing local hours into fake dollars -- would report "you spent $3.20"
to somebody who ran on free hardware, which is a lie, and would silently rewrite
everyone's history every time the rate was retuned.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
from enum import StrEnum
from zoneinfo import ZoneInfo


class Currency(StrEnum):
    """What a unit of capacity is denominated in."""

    USD = "USD"
    GPU_HOUR = "GPU_HOUR"

    @property
    def symbol(self) -> str:
        return "$" if self is Currency.USD else ""

    @property
    def suffix(self) -> str:
        return "" if self is Currency.USD else " gpu-hr"

    @property
    def quantum(self) -> Decimal:
        """Smallest unit worth tracking. Cents, or hundredths of a GPU-hour."""
        return Decimal("0.01")


def money(value: Decimal | int | float | str) -> Decimal:
    """Coerce to a Decimal without ever routing through binary float.

    `Decimal(0.1)` is 0.1000000000000000055511151231257827. `money(0.1)` is
    `Decimal("0.1")`. Every number that enters the ledger goes through here.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(repr(value))
    return Decimal(str(value))


def quantize(value: Decimal, currency: Currency) -> Decimal:
    """Round to the currency's smallest unit, half-up, the way a human would."""
    return money(value).quantize(currency.quantum, rounding=ROUND_HALF_UP)


def fmt(value: Decimal | int | float, currency: Currency) -> str:
    """Render for a human. `$12.00` or `4.00 gpu-hr`."""
    amount = quantize(money(value), currency)
    return f"{currency.symbol}{amount:,.2f}{currency.suffix}"


def billing_period(moment: dt.datetime, timezone: str) -> str:
    """The `YYYY-MM` bucket a moment falls in, in the club's local timezone.

    A job submitted at 11pm on January 31st in San Diego belongs to January,
    even though it is already February in UTC. Getting this wrong means the
    monthly budget resets at the wrong hour, which somebody would notice.
    """
    local = moment.astimezone(ZoneInfo(timezone))
    return f"{local.year:04d}-{local.month:02d}"


ZERO = Decimal("0")
