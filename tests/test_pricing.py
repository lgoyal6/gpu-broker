"""Prices, refreshed from AWS rather than typed in once and left to rot.

moto does not implement the Price List API at all -- `get_products` answers
"Not yet implemented" -- so the client here is a hand-written double. It returns
the real response shape, which is the part the parser has to survive: opaque SKU
keys at two levels, several listings per instance type, and prices as strings.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from gpu_broker.money import Currency
from gpu_broker.pricing import PriceBook, PricingError, fetch_on_demand


def listing(usd: str, unit: str = "Hrs") -> str:
    """One Price List entry, in the shape AWS actually returns."""
    return json.dumps(
        {
            "product": {"attributes": {"instanceType": "g5.xlarge"}},
            "terms": {
                "OnDemand": {
                    "ABC123.JRTCKXETXF": {
                        "priceDimensions": {
                            "ABC123.JRTCKXETXF.6YS6EN2CT7": {
                                "unit": unit,
                                "pricePerUnit": {"USD": usd},
                            }
                        }
                    }
                }
            },
        }
    )


class PricingDouble:
    def __init__(self, listings=None, error=None):
        self.listings = listings if listings is not None else [listing("1.0060000000")]
        self.error = error
        self.calls: list[dict] = []

    def get_products(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return {"PriceList": list(self.listings)}


# ------------------------------------------------------------------ fetching


def test_a_price_comes_back_as_a_decimal():
    assert fetch_on_demand(PricingDouble(), "g5.xlarge", "us-west-2") == Decimal("1.0060000000")


def test_the_query_is_filtered_enough_to_be_unambiguous():
    """Without these, the same instance type comes back as Windows and Linux,
    dedicated and shared, with and without preinstalled software. Picking the
    first at random is how a price table quietly quotes Windows rates."""
    client = PricingDouble()
    fetch_on_demand(client, "g5.xlarge", "us-west-2")

    fields = {f["Field"]: f["Value"] for f in client.calls[0]["Filters"]}
    assert fields["instanceType"] == "g5.xlarge"
    assert fields["operatingSystem"] == "Linux"
    assert fields["tenancy"] == "Shared"
    assert fields["preInstalledSw"] == "NA"
    assert fields["capacitystatus"] == "Used"


def test_the_region_is_a_code_not_a_prose_name():
    """`location` needs a map from us-west-2 to 'US West (Oregon)', and that map
    is wrong every time AWS opens a region."""
    client = PricingDouble()
    fetch_on_demand(client, "g5.xlarge", "us-west-2")
    fields = {f["Field"]: f["Value"] for f in client.calls[0]["Filters"]}
    assert fields["regionCode"] == "us-west-2"
    assert "location" not in fields


def test_several_surviving_listings_take_the_lowest():
    client = PricingDouble([listing("5.00"), listing("1.01"), listing("3.00")])
    assert fetch_on_demand(client, "g5.xlarge", "us-west-2") == Decimal("1.01")


def test_non_hourly_dimensions_are_ignored():
    client = PricingDouble([listing("99.00", unit="Quantity")])
    with pytest.raises(PricingError, match="no usable price"):
        fetch_on_demand(client, "g5.xlarge", "us-west-2")


def test_an_empty_listing_says_what_to_check():
    client = PricingDouble([])
    with pytest.raises(PricingError, match="unavailable there, or the name is wrong"):
        fetch_on_demand(client, "g5.xlarge", "us-west-2")


def test_an_api_failure_is_a_pricing_error_not_a_crash():
    client = PricingDouble(error=RuntimeError("connection reset"))
    with pytest.raises(PricingError, match="did not answer"):
        fetch_on_demand(client, "g5.xlarge", "us-west-2")


def test_malformed_json_does_not_take_down_a_refresh():
    """This is somebody else's JSON. A KeyError here would lose four good prices
    because the fifth was odd."""
    client = PricingDouble(["not json at all", listing("1.01")])
    assert fetch_on_demand(client, "g5.xlarge", "us-west-2") == Decimal("1.01")


# ----------------------------------------------------------------- the book


def test_the_builtin_table_works_with_no_aws_at_all(config, clock):
    book = PriceBook(config, clock)
    assert book.hourly("a10g") == Decimal("1.006")
    assert book.price("a10g").source == "builtin"


def test_a_refresh_marks_prices_as_coming_from_aws(config, clock):
    book = PriceBook(config, clock)
    report = book.refresh(PricingDouble([listing("2.50")]))

    assert report.ok
    assert book.hourly("a10g") == Decimal("2.50")
    assert book.price("a10g").source == "aws"
    assert book.price("a10g").region == config.aws.region


def test_free_capacity_is_never_priced_against_aws(config, clock):
    """An hour on the lab machine costs an hour. AWS has no opinion."""
    book = PriceBook(config, clock)
    client = PricingDouble()
    book.refresh(client)

    assert book.price("a6000").currency is Currency.GPU_HOUR
    assert book.price("a6000").source == "builtin"
    assert all("a6000" not in str(call) for call in client.calls)


def test_one_bad_instance_type_does_not_lose_the_good_ones(config, clock):
    class Flaky(PricingDouble):
        def get_products(self, **kwargs):
            fields = {f["Field"]: f["Value"] for f in kwargs["Filters"]}
            if fields["instanceType"] == "p4d.24xlarge":
                return {"PriceList": []}
            return {"PriceList": [listing("1.01")]}

    book = PriceBook(config, clock)
    report = book.refresh(Flaky())

    assert not report.ok
    assert [name for name, _ in report.failed] == ["a100"]
    assert len(report.updated) == 3
    assert book.price("a100").source == "builtin", "a failed refresh overwrote a price"


def test_builtin_prices_are_reported_as_stale(config, clock):
    book = PriceBook(config, clock)
    stale = {price.gpu_type for price in book.stale()}
    assert "a10g" in stale
    assert "a6000" not in stale, "free capacity does not go stale"


def test_a_fresh_aws_price_is_not_stale(config, clock):
    book = PriceBook(config, clock)
    book.refresh(PricingDouble())
    assert {p.gpu_type for p in book.stale()} == set()


def test_prices_age(config, clock):
    book = PriceBook(config, clock)
    book.refresh(PricingDouble())
    clock.advance(days=45)
    assert {p.gpu_type for p in book.stale(older_than_days=30)} == {"t4", "a10g", "l4", "a100"}


# -------------------------------------------------------------- persistence


def test_a_refreshed_price_survives_a_restart(broker, state_dir, clock, cloud, lab):
    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    broker.refresh_prices(PricingDouble([listing("2.50")]))
    assert broker.prices.hourly("a10g") == Decimal("2.50")
    broker.close()

    restarted = Broker.open(state_dir, clock=clock, backends=[cloud, lab], config=load_config(state_dir))
    assert restarted.prices.hourly("a10g") == Decimal("2.50")
    assert restarted.prices.price("a10g").source == "aws"
    restarted.close()


def test_a_price_for_a_gpu_type_nobody_configured_is_not_resurrected(broker, clock):
    """Removing a type from config should remove it, not have it come back
    because a stale row is still on disk."""
    from gpu_broker.models import Price

    broker.store.save_prices(
        [Price("h100", Decimal("9.99"), Currency.USD, "aws", clock.now())]
    )
    broker.prices.load(broker.store.prices())
    assert "h100" not in broker.prices.all()


# ------------------------------------------------- the price the club is charged


def test_a_refreshed_price_governs_what_a_job_reserves(broker, users):
    """A refresh that nobody's budget can see is a display feature.

    `gpu prices` printed $3.00/h while admission went on reserving at the
    builtin $1.006/h, so the budget was enforced against a number the club is
    not charged. The reservation has to move with the price.
    """
    broker.refresh_prices(PricingDouble([listing("3.0000000000")]))
    assert broker.prices.hourly("a10g") == Decimal("3.0000000000")

    job = broker.submit(
        user_id="ana", command="python train.py", gpu_type="a10g", hours=2.0
    ).job
    # 2h of run time plus the 0.25h startup allowance, at the refreshed price.
    assert job.reserved == Decimal("6.75")


def test_a_refreshed_price_governs_what_a_running_job_is_charged(broker, users, clock):
    """Accrual read the same stale table admission did, so a job that AWS was
    billing at $3.00/h settled at $1.006/h and the ledger under-reported the
    club's real spend for as long as the job ran."""
    broker.refresh_prices(PricingDouble([listing("3.0000000000")]))
    broker.submit(
        user_id="ana", command="python train.py", gpu_type="a10g", hours=4.0,
        budget="20.00",
    )
    broker.tick()
    clock.advance(hours=1)
    broker.tick()

    spent = broker.budgets("ana")[Currency.USD].spent
    assert spent == Decimal("3.00"), f"one hour at $3.00/h settled as {spent}"


def test_a_refreshed_price_survives_into_the_money_path_after_a_restart(
    broker, state_dir, clock, cloud, lab
):
    """The price is reloaded from disk at startup, so the restart must not
    quietly put admission back on the builtin table."""
    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    broker.refresh_prices(PricingDouble([listing("3.0000000000")]))
    broker.add_user("ana")
    broker.close()

    restarted = Broker.open(
        state_dir, clock=clock, backends=[cloud, lab], config=load_config(state_dir)
    )
    job = restarted.submit(
        user_id="ana", command="python train.py", gpu_type="a10g", hours=2.0
    ).job
    assert job.reserved == Decimal("6.75")
    restarted.close()


def test_a_price_the_refresh_could_not_reach_stays_on_the_builtin_number(broker, users):
    """Partial failure is the normal case. A type AWS would not price keeps the
    builtin number rather than becoming unpriceable, and says so."""
    broker.refresh_prices(PricingDouble(error=RuntimeError("pricing API down")))
    assert broker.prices.price("a10g").source == "builtin"

    job = broker.submit(
        user_id="ana", command="python train.py", gpu_type="a10g", hours=2.0
    ).job
    assert job.reserved == Decimal("2.26")  # 2.25h at the builtin $1.006
