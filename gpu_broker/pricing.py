"""What capacity costs, refreshed from AWS rather than hardcoded.

A price table somebody typed in once is wrong within a quarter, and wrong in the
direction that matters: every budget check, every refusal, and every "dollars
per useful GPU-hour" number downstream inherits the error silently.

So prices come from the AWS Price List API and are stored with their source and
their age. `gpu prices` prints both. A number nobody has refreshed since March
is a different kind of number from one AWS gave us this morning, and the club
should be able to see which it is looking at.

The built-in table is the floor, not the plan. It exists so the broker still
works on a laptop with no AWS account, and every row from it says `builtin`.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .clock import Clock
from .config import BrokerConfig
from .errors import BrokerError
from .models import Price
from .money import Currency, money


class PricingError(BrokerError):
    """The pricing API could not be reached or did not answer usefully."""


@dataclass(frozen=True)
class RefreshReport:
    at: dt.datetime
    updated: tuple[Price, ...] = ()
    failed: tuple[tuple[str, str], ...] = ()

    @property
    def ok(self) -> bool:
        return bool(self.updated) and not self.failed


class PriceBook:
    """Every price the broker knows, and where each came from.

    Reads come from memory; the store is the durable copy. Nothing here calls
    AWS unless `refresh` is called explicitly, so a slow or unavailable pricing
    API can never block admitting a job.
    """

    def __init__(self, config: BrokerConfig, clock: Clock) -> None:
        self.config = config
        self.clock = clock
        self._prices: dict[str, Price] = {}
        self._load_builtin()

    def _load_builtin(self) -> None:
        now = self.clock.now()
        for gpu in self.config.gpu_types:
            self._prices[gpu.name] = Price(
                gpu_type=gpu.name,
                hourly=gpu.hourly_price,
                currency=gpu.currency,
                source="builtin",
                priced_at=now,
                instance_type=self.config.aws.instance_types.get(gpu.name, ""),
                region=self.config.aws.region,
            )

    def load(self, stored: dict[str, Price]) -> None:
        """Overlay what was saved. Unknown gpu types are ignored rather than
        resurrected: a type somebody removed from config should not come back
        because a price for it is still on disk."""
        for gpu_type, price in stored.items():
            if gpu_type in self._prices:
                self._prices[gpu_type] = price

    def all(self) -> dict[str, Price]:
        return dict(self._prices)

    def price(self, gpu_type: str) -> Price:
        try:
            return self._prices[gpu_type]
        except KeyError:
            raise BrokerError(f"no price for gpu type {gpu_type!r}") from None

    def hourly(self, gpu_type: str) -> Decimal:
        return self.price(gpu_type).hourly

    def stale(self, older_than_days: float = 30.0) -> list[Price]:
        """Prices worth refreshing. Free capacity is never stale -- an hour on
        the lab machine costs an hour, permanently."""
        now = self.clock.now()
        return [
            price
            for price in self._prices.values()
            if price.currency is Currency.USD
            and (price.source == "builtin" or price.age_days(now) > older_than_days)
        ]

    # ---------------------------------------------------------------- refresh

    def refresh(self, pricing_client: Any) -> RefreshReport:
        """Pull on-demand prices for every paid GPU type from AWS.

        Partial success is the normal case: one instance type that AWS has no
        listing for should not throw away the four that came back fine.
        """
        now = self.clock.now()
        updated: list[Price] = []
        failed: list[tuple[str, str]] = []

        for gpu in self.config.gpu_types:
            if gpu.currency is not Currency.USD:
                continue  # free capacity has no list price
            instance_type = self.config.aws.instance_types.get(gpu.name)
            if not instance_type:
                failed.append((gpu.name, "no instance type mapped in aws.instance_types"))
                continue
            try:
                hourly = fetch_on_demand(
                    pricing_client, instance_type, self.config.aws.region
                )
            except PricingError as exc:
                failed.append((gpu.name, str(exc)))
                continue

            price = Price(
                gpu_type=gpu.name,
                hourly=hourly,
                currency=Currency.USD,
                source="aws",
                priced_at=now,
                instance_type=instance_type,
                region=self.config.aws.region,
            )
            self._prices[gpu.name] = price
            updated.append(price)

        return RefreshReport(at=now, updated=tuple(updated), failed=tuple(failed))


def fetch_on_demand(pricing_client: Any, instance_type: str, region: str) -> Decimal:
    """One instance type's Linux on-demand price, from the Price List API.

    Filtered on `regionCode` rather than `location`, which avoids needing a map
    from `us-west-2` to the string "US West (Oregon)" -- a map that is wrong
    every time AWS opens a region.

    The other four filters are not optional. Without them the same instance type
    comes back several times over: Windows and Linux, dedicated and shared,
    with and without preinstalled software, and `capacitystatus` of `Used` versus
    a reservation listing. Picking the first of those at random is how a price
    table ends up quietly quoting Windows rates.
    """
    filters = [
        {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
        {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
        {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
        {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
        {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
        {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
    ]
    try:
        response = pricing_client.get_products(
            ServiceCode="AmazonEC2", Filters=filters, MaxResults=100
        )
    except Exception as exc:  # noqa: BLE001 - botocore, urllib, anything
        raise PricingError(f"the pricing API did not answer: {exc}") from exc

    listings = response.get("PriceList", [])
    if not listings:
        raise PricingError(
            f"AWS lists no on-demand price for {instance_type} in {region}. "
            "Either the type is unavailable there, or the name is wrong"
        )

    prices = [price for price in (_extract(entry) for entry in listings) if price is not None]
    if not prices:
        raise PricingError(f"the listing for {instance_type} had no usable price")
    # Several SKUs can survive the filters. The lowest is the plain on-demand
    # rate; anything above it carries a term nobody asked for.
    return min(prices)


def _extract(entry: Any) -> Decimal | None:
    """Dig the hourly USD rate out of one Price List entry.

    The shape is `terms.OnDemand.<sku>.priceDimensions.<id>.pricePerUnit.USD`,
    with opaque keys at two levels. Defensive throughout: this is somebody
    else's JSON and a `KeyError` here would take down a price refresh.
    """
    try:
        document = json.loads(entry) if isinstance(entry, str) else entry
    except (TypeError, ValueError):
        return None
    if not isinstance(document, dict):
        return None

    on_demand = document.get("terms", {}).get("OnDemand", {})
    found: list[Decimal] = []
    for term in on_demand.values():
        for dimension in term.get("priceDimensions", {}).values():
            if dimension.get("unit", "").lower() not in ("hrs", "hours", "hour"):
                continue
            raw = dimension.get("pricePerUnit", {}).get("USD")
            if raw in (None, ""):
                continue
            try:
                value = money(raw)
            except Exception:  # noqa: BLE001 - malformed number
                continue
            if value > 0:
                found.append(value)
    return min(found) if found else None
